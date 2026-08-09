package node

import (
	"context"
	"errors"
	"strings"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/rs/zerolog/log"
)

// datagram_integration.go is the wire-level wiring of the datagram transport
// layer into the node: the two capabilities of §6, the raw advertised
// capability set and the `dtypes` set of §6.1, and the single ingress every
// arriving `datagram` frame passes through, on BOTH network directions.
//
// What is deliberately NOT here, and lives in its own file for a reason:
// constructing datagram.Pipeline and driving its schedules (datagram_layer.go),
// and the resolver / emitter adapters the layer is wired from
// (datagram_resolver.go). This file owns only what the WIRE sees, so a raw line
// reaches the strict parser byte-for-byte on both directions and both sides
// know each other's capabilities and types — with or without a conveyor behind
// the ingress.
//
// Reference: docs/refactoring/datagram-transport.md §2.2, §2.3, §3.4, §4.4,
// §6, §6.1.

// ---------------------------------------------------------------------------
// Local advertisement (§6)
// ---------------------------------------------------------------------------

// datagramAdvertise is what THIS node claims about the datagram plane. The
// two flags are separate because the two capabilities are separate
// statements (§6): "I can be an endpoint" and "I will carry other people's
// datagrams" are different promises, and a client node keeps the second one
// to itself so an honest neighbour never picks it as a relay.
type datagramAdvertise struct {
	// Endpoint advertises mesh_datagram_v1.
	Endpoint bool
	// Transit advertises mesh_datagram_transit_v1. Only ever true for a
	// node that really forwards.
	Transit bool
}

// datagramPlaneCapability is what this BUILD can serve: the feature flag and
// the node type, both immutable after New, and nothing else.
//
// The whole plane sits behind cfg.EnableDatagramV1 (default ON, with
// CORSA_ENABLE_DATAGRAM_V1 as the kill switch — enableDatagramV1FromEnv): a
// node that does not advertise mesh_datagram_v1 is never sent a datagram, so
// turning the flag off makes the layer invisible to the network. Transit
// additionally requires a full node — forwarding is exactly what a client node
// does not do, and advertising a promise this node will not keep would strand
// frames on it.
//
// The ENDPOINT half depends on the flag and on nothing else — least of all on
// the type registry. mesh_datagram_v1 states exactly one thing (§6): this node
// understands the envelope and can receive a datagram addressed to it at the
// TRANSPORT level, instead of answering unknown_command and closing the
// connection. That is true the moment the layer is wired, whatever the
// registry holds. WHICH types the node can handle is stated by `dtypes` alone
// (§6.1), and an empty registry states it there, as an explicitly empty set.
//
// Tying the endpoint half to the registry is what used to leave the plane
// unable to carry anything at all: the candidate filter demands
// mesh_datagram_v1 from EVERY candidate, transit included (§2.2 rule 2,
// §4.3), so a network of PR-0 nodes with empty registries had no admissible
// next hop for anybody's frame — not even a purely transit one.
//
// This is the value the PIPELINE's own gates are built from
// (localAdvertisedCapabilities), and it deliberately does NOT read whether the
// Service already holds a layer. The gates' snapshot is taken WHILE that layer
// is being constructed, so a value that answered "no plane yet" would be frozen
// into the pipeline and leave the self-gate and the transit gate refusing
// everything for the lifetime of the process. What the wire claims is stated by
// localDatagramAdvertise below, which reads both.
func (s *Service) datagramPlaneCapability() datagramAdvertise {
	if !s.cfg.EnableDatagramV1 {
		return datagramAdvertise{}
	}
	return datagramAdvertise{
		Endpoint: true,
		Transit:  !s.NodeType().IsClient(),
	}
}

// localDatagramAdvertise resolves what this Service TELLS THE NETWORK, and it
// is the single source of that truth: the hello and welcome frames, the
// negotiated capability set of both directions, and the `dtypes` field all read
// this one function.
//
// It is what the build can serve AND whether a conveyor was really assembled:
// construction can fail (NewService then clears the flag), and a node that
// advertised over a plane that does not exist would go on being chosen by
// neighbours as an endpoint and as a relay while dropping every frame they sent
// it — a black hole that looks, from outside, exactly like a healthy node.
//
// There is no state in which a node with a live plane withholds these names.
// The one transition that used to produce such a state — a recovery barrier
// that refused to open until a startup pass over durable stores had finished —
// is gone together with the durable coordination it guarded, and with it the
// only path by which this node ever stopped claiming mesh_datagram_v1.
func (s *Service) localDatagramAdvertise() datagramAdvertise {
	if !s.datagramPlaneReady() {
		return datagramAdvertise{}
	}
	return s.datagramPlaneCapability()
}

// localHandshakeCapabilityNames is the COMPLETE raw capability set this node
// puts on the wire: the role names localCapabilities builds, and nothing else.
//
// There used to be a second half — the PROFILE capabilities a registry honoured
// at that instant — and it went with the registry. A stateless forwarder
// advertises what it IS (an endpoint, a transit) and the types it implements;
// the versions and options of an endpoint protocol are that protocol's business
// and travel inside its own frames, not in a path-wide requirement every relay
// on the way has to match.
//
// It is the single source both halves of the handshake read — the hello and the
// welcome — so the two can never claim different things about this node.
//
// Nothing records the result per connection any more. The record existed for
// the fail-closed transition that had to find the sessions on which THIS node
// advertised a name it was withdrawing, and a stateless forwarder withdraws
// nothing: the set moves only when the plane itself starts or stops.
func (s *Service) localHandshakeCapabilityNames() []domain.CapabilityName {
	roles := localCapabilities(s.cfg.EnableMeshRoutingV3, s.localDatagramAdvertise())

	names := make([]domain.CapabilityName, 0, len(roles))
	for _, capability := range roles {
		names = append(names, domain.CapabilityName(capability))
	}
	return names
}

// localHandshakeCapabilityStrings renders the same set for the hello/welcome
// frame, where Frame.Capabilities is []string.
func localHandshakeCapabilityStrings(names []domain.CapabilityName) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = name.String()
	}
	return out
}

// localDatagramDTypes is the dtype set this build handles as an ENDPOINT —
// the value of the `dtypes` handshake field (§6.1).
//
// It is read from the REAL registry, never from a constant. The registry is
// the single place that knows which types have a handler, and deriving the
// declaration from anything else is how a node comes to advertise types it
// does not implement — visible in the diagnostic as a non-empty `"dtypes"`
// next to `"registered_dtypes": []`.
func (s *Service) localDatagramDTypes() []domain.DType {
	layer := s.datagramLayer()
	if layer == nil {
		return nil
	}
	return layer.types.DTypes()
}

// localDTypeStrings renders the declared set into the optional `dtypes` field
// of the hello/welcome frame — nil for the absent form, a pointer to the
// (possibly empty) name list otherwise.
//
// An ENDPOINT always emits the field, in full, EMPTY INCLUDED. The field IS
// the set (§6.1): an empty registry is emitted as an explicitly empty array —
// this node speaks the envelope (it advertises mesh_datagram_v1, it accepts
// and forwards frames) and has a handler for no type — and a non-empty one is
// emitted whole, because a partial list would both understate what this node
// handles and disagree with the identity record's `dtypes`, which carries the
// same set by contract.
//
// There is no set whose omission "says the same thing". Omitting the field for
// a set that happened to match a declared baseline was the shorthand this
// layer used for one unreleased version, and it only worked while a reader was
// willing to credit a silent peer with four handlers — which is exactly the
// reading that was removed.
//
// A node that does not advertise mesh_datagram_v1 emits no field: with the
// flag off there is no layer, no envelope and nothing to declare.
func (s *Service) localDTypeStrings(advertise datagramAdvertise) *[]string {
	if !advertise.Endpoint {
		return domain.AbsentDTypes().WireField()
	}
	return domain.ExplicitDTypes(s.localDatagramDTypes()).WireField()
}

// ---------------------------------------------------------------------------
// Peer declarations (§2.2, §6.1)
// ---------------------------------------------------------------------------

// declarationsFromHandshake validates a peer's hello/welcome into the raw
// state a session keeps beside its typed capability set.
//
// Both halves degrade rather than fail, and they degrade to DIFFERENT
// values because the spec assigns them different meanings:
//
//   - a capability list breaching its bounds empties the WHOLE raw set. The
//     peer stops being a datagram candidate — the role gate reads this set;
//     the typed set is untouched and the session lives on;
//   - a dtypes list breaching its bounds is read as an ABSENT field, hence
//     as no declared type at all (§6.1) — conservative degradation, never a
//     torn-down handshake. The peer stops being a lawful DESTINATION for any
//     type while remaining a lawful transit hop.
//
// Neither branch returns an error, and that is the contract, not an
// omission: the caller has no lawful reaction other than to carry on.
//
// "Fixed for the lifetime of the session" (§6.1) is enforced by the call
// sites, not here: outbound sessions run applyWelcomeMetadata exactly once,
// and the inbound path refuses a second hello as soon as auth is initiated
// (the re-hello guard in dispatchNetworkFrame). A pre-auth re-hello can still
// replace the values, which is the same window in which the TYPED capability
// set is replaceable — there is no session yet to be inconsistent with.
func declarationsFromHandshake(frame protocol.Frame) netcore.HandshakeDeclarations {
	return netcore.HandshakeDeclarations{
		AdvertisedNames: domain.ParseRawCapabilityNames(frame.Capabilities),
		DeclaredDTypes:  domain.ParseDeclaredDTypesField(frame.DTypes),
	}
}

// datagramAdvertisedCapabilities projects the stored raw set into the layer
// type that answers the candidate filter.
//
// The names have already been validated once, on arrival; passing them
// through datagram.NewAdvertisedCapabilities again is idempotent and keeps
// ONE validator in the tree — the layer's constructor and the node's parser
// are the same function underneath (domain.ParseRawCapabilityNames).
func datagramAdvertisedCapabilities(declarations netcore.HandshakeDeclarations) datagram.AdvertisedCapabilities {
	names := make([]string, len(declarations.AdvertisedNames))
	for i, name := range declarations.AdvertisedNames {
		names[i] = name.String()
	}
	return datagram.NewAdvertisedCapabilities(names)
}

// datagramDeclaredDTypes projects the stored dtype set into the layer type.
// The stored value already carries the validated names, so the projection is
// a lookup-form change and never a re-interpretation — the one place that
// could quietly credit a peer with a type it never listed.
func datagramDeclaredDTypes(declarations netcore.HandshakeDeclarations) datagram.DeclaredDTypes {
	return datagram.NewDeclaredDTypes(declarations.DeclaredDTypes)
}

// connDeclarations returns the raw handshake declarations of an INBOUND
// connection. Reads NetCore, the single source of truth for live connection
// state, exactly as connHasCapability does for the typed set.
func (s *Service) connDeclarations(id domain.ConnID) netcore.HandshakeDeclarations {
	pc := s.netCoreForID(id)
	if pc == nil {
		return netcore.HandshakeDeclarations{}
	}
	return pc.Declarations()
}

// sessionDeclarations returns the raw handshake declarations mirrored onto an
// OUTBOUND peer session. The session mirror exists for the same reason
// session.capabilities does: the outbound dispatcher is addressed by
// PeerAddress and has no ConnID to reach NetCore with.
func (s *Service) sessionDeclarations(address domain.PeerAddress) netcore.HandshakeDeclarations {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	session := s.resolveSessionLocked(address)
	if session == nil {
		return netcore.HandshakeDeclarations{}
	}
	return session.declarations.Clone()
}

// ---------------------------------------------------------------------------
// Ingress (§2.3, §3.4, §4.4)
// ---------------------------------------------------------------------------

// datagramDirection names which of the two network paths a frame arrived on.
// Both funnel into handleDatagramFrame, and the value exists so a log or a
// metric can still tell them apart — the receive budget differs (the inbound
// command reader caps at 128 KiB, the peer-session reader at 8 MiB), and a
// size refusal that cannot say which side it came from is not actionable.
type datagramDirection uint8

const (
	// datagramDirectionUnset is the zero value; never a valid argument.
	datagramDirectionUnset datagramDirection = iota
	// datagramInbound is an accepted TCP connection's command plane.
	datagramInbound
	// datagramOutboundSession is a dialled peer session's read loop.
	datagramOutboundSession
)

var datagramDirectionNames = map[datagramDirection]string{
	datagramDirectionUnset:  "unset",
	datagramInbound:         "inbound",
	datagramOutboundSession: "outbound_session",
}

// String returns the log label of the direction.
func (d datagramDirection) String() string {
	if name, ok := datagramDirectionNames[d]; ok {
		return name
	}
	return "unknown"
}

// datagramIngressResult is what the ingress reports back to the dispatcher
// that called it.
//
// Ban travels as its own field rather than as a class of refusals because
// §4.4 draws the line by WHAT was violated, not by how bad it looked: the
// stable header and auth — a fingerprint mismatch, a forged signature, a
// field out of bounds — are what every datagram transit is obliged to check, so
// getting them wrong is punishable. An unknown dtype and application-level
// garbage are not: the layer explicitly allows an honest node to relay a
// type it cannot read, so punishing it would hit exactly the well-behaved
// baseline relays the layer depends on.
type datagramIngressResult struct {
	err      error
	accepted bool
	ban      bool
}

// Accepted reports whether the frame passed the wire-level gates.
func (r datagramIngressResult) Accepted() bool { return r.accepted }

// BanWorthy reports whether the neighbour should be charged ban points.
func (r datagramIngressResult) BanWorthy() bool { return r.ban }

// Err returns the cause behind a refusal, nil on acceptance.
func (r datagramIngressResult) Err() error { return r.err }

// exceedsDatagramFrameLine reports whether a line breaches the strict
// 128 KiB budget of §2.3. The budget itself is wireLineBudget (admission.go),
// shared with the announce plane, which obeys the same 128 KiB ceiling on the
// same two readers.
func exceedsDatagramFrameLine(line string) bool {
	return wireLineBudget(line) > protocol.MaxFrameLine
}

// countOversizeDatagramRefusal records a datagram turned away on the strict
// budget by the POST-PARSE gate of the peer-session reader
// (refuseOversizeFrameLine), which judges the type protocol.ParseFrameLine
// resolved and therefore never reaches this plane's own refusal path.
//
// Without it the frame would leave no trace at all, so §10's "dropped by
// reason" ledger would be missing a refusal the widest reader on the node can
// produce. The reason is the same DropFrameTooLarge the ingress uses, because
// it is the same gate applied at a different point — a second reason for one
// verdict would make the two counters impossible to add up.
//
// It is a no-op for the announce-plane types that share the budget: they have
// no per-reason ledger of their own, and inventing one for them here would put
// routing accounting inside the datagram counter series.
func (s *Service) countOversizeDatagramRefusal(frameType string) {
	if frameType != protocol.DatagramFrameType {
		return
	}
	s.datagramMetrics.ObserveInbound("", datagram.InboundDropped, datagram.DropFrameTooLarge)
}

// ---------------------------------------------------------------------------
// The size rule of §2.3, stated once
// ---------------------------------------------------------------------------

// oversizeDatagramResyncBytes is how far PAST protocol.MaxFrameLine a reader
// will follow a line that claims this plane, so it can find the newline and go
// on reading the connection.
//
// One frame of slack is the whole allowance, and it is derived rather than
// chosen: §2.3 caps a datagram at MaxFrameLine, so anything a conforming sender
// anywhere on the path could have produced is already in hand when the reader
// stops. Beyond this bound the bytes are not an over-sized FRAME any more, they
// are an unterminated stream — which is the immediate neighbour's own transport
// behaviour and not a property of anything it relayed — and a stream stays fatal
// for the connection exactly as it is for every other frame type.
//
// It also keeps the charge below meaningful: what a refused line bills is what
// the two stages actually read, which is at most MaxFrameLine + this plus the
// part of one buffer fill each stage stops on — still far inside the §5 byte
// burst, so the bucket is really drained instead of the charge being refused
// whole for exceeding it.
const oversizeDatagramResyncBytes = protocol.MaxFrameLine

// refuseOversizeDatagram is the SINGLE statement of what a datagram past
// protocol.MaxFrameLine costs, and every path that can refuse one ends here: the
// two readers that stop mid-line and judge from the CLAIM, and the ingress that
// measures the complete line.
//
// The price is one DropFrameTooLarge in the §10 ledger and a log line. NOT a ban
// score, not a rate-limit verdict, not a violation on a session ledger, and
// never a closed connection — and the reason is the same one §4.4 gives for
// every other refusal it keeps ban-free: the neighbour that handed the line over
// is not the author of the frame inside it. A relay forwards what it was given;
// nothing in the envelope obliges it to have measured the frame the way this
// node does, and punishing it hits exactly the honest baseline relays the plane
// depends on. The frame is still refused — the limit is what protects the
// reader, and it is not weakened here.
//
// What makes the silence affordable is the CHARGE, which is the callers'
// business: the ingress has already billed §5 on the raw line before it reaches
// this verdict, and the pre-parse callers bill it through
// refuseOversizeDatagramClaim below. A refusal nobody paid for would be the one
// free channel on the node.
func (s *Service) refuseOversizeDatagram(
	direction datagramDirection,
	key datagram.AdmissionKey,
	size int,
) datagramIngressResult {
	log.Warn().
		Str("direction", direction.String()).
		Str("budget_key", key.String()).
		Int("size", size).
		Int("limit", protocol.MaxFrameLine).
		Msg("datagram_frame_too_large_dropped")
	return s.refuseDatagram(direction, datagram.DropFrameTooLarge, false, protocol.ErrFrameTooLarge)
}

// refuseOversizeDatagramClaim is the PRE-PARSE entry to that rule, for a reader
// that stopped in the middle of a line and will never hand it to the ingress. It
// reports whether this plane took the line; false leaves the caller its ordinary
// refusal.
//
// # Why a CLAIM may decide it
//
// Nothing here has a classification: no complete line was read, so the only name
// the bytes ever had is the one their first bytes gave
// (claimedFrameTypeFromPrefix). That claim is trusted for exactly two things —
// moving the charge from the response plane's shared bucket to this plane's own,
// NARROWER §5 one, and dropping a punishment §2 and §4.4 forbid for a size
// verdict in the first place. It cannot buy processing, because the line is
// refused either way, so unlike the budget replacement of
// sessionDatagramPaysItsOwnBudget there is no keyword here that turns a meter
// off: prepending `{"type":"datagram",` moves a neighbour onto a stricter budget
// and nothing else.
//
// # Why the charge comes first
//
// §4.1 step 1 puts admission before any decoding, and this refusal is the
// cheapest verdict on the widest reader of the node — the classic shape of a
// free flood. The bytes this node was made to READ are billed before the
// verdict, so a stream of oversize claims drains the very bucket a stream of
// legal datagrams would, and drains the sender's own plane with it. The verdict
// of the charge is deliberately ignored: it is not a second decision, the line
// is refused whichever way it goes.
//
// With no layer there is no §5 budget to move the cost to, so the claim buys
// nothing at all and the caller keeps its own refusal — the same "no layer, no
// exemption" condition datagramCarriesOwnBudget carries. Likewise for a
// neighbour with no billable key: the charge would land nowhere, and a free
// refusal is exactly what must not exist here.
func (s *Service) refuseOversizeDatagramClaim(
	direction datagramDirection,
	claimed string,
	key datagram.AdmissionKey,
	size int,
) bool {
	layer := s.datagramLayer()
	if claimed != protocol.DatagramFrameType || layer == nil || key.IsZero() {
		return false
	}
	_ = layer.admission.Admit(key, size)
	s.refuseOversizeDatagram(direction, key, size)
	return true
}

// countAmbiguousDatagramRefusal records a line refused for naming its type
// ambiguously, when the best-effort attribution says it claimed to be a
// datagram.
//
// The reason is DropMalformed and not a new one, because it is the SAME verdict
// the strict parser of §3.4 would have reached one step later: a duplicate
// top-level key is malformed for this plane by contract. Before the refusal
// moved above the parser, that is exactly where such a line was counted, and
// changing the reason with the position would have made a §10 ledger that no
// longer adds up across the change.
//
// The attribution is peekFrameType and is best-effort BY CONSTRUCTION — the
// line refused to name itself, so no scan can do better. That cuts both ways
// and the direction is deliberate: a decoy naming `datagram` first lands on
// this counter without having been one, and a decoy hiding it lands on no
// counter at all. Neither costs anything, because the DECISION was already
// taken from the classification and this call only labels it.
func (s *Service) countAmbiguousDatagramRefusal(line string) {
	if peekFrameType(line) != protocol.DatagramFrameType {
		return
	}
	s.datagramMetrics.ObserveInbound("", datagram.InboundDropped, datagram.DropMalformed)
}

// isDatagramWireLine reports whether a raw line is UNAMBIGUOUSLY a datagram,
// judged before anything is parsed.
//
// It is the classification both receive paths divert on, and it exists because
// §4.1 step 1 puts the neighbour's byte and frame budget "before any decoding":
// a line that reaches protocol.ParseFrameLine has already cost this node a full
// JSON unmarshal that nobody charged for. Diverting on this predicate means a
// datagram is charged by the layer first and never meets the universal parser
// at all — the strict parser of §3.4 has to read the original bytes anyway.
//
// It answers from topLevelFrameType and not from peekFrameType, which is the
// same correctness argument the pre-parse budget makes: peekFrameType reports
// the first `"type"` anywhere in the line, encoding/json reports the last
// TOP-LEVEL one, and a sender picks which reader gets which answer. A line that
// does not name its type unambiguously is simply not diverted — it keeps the
// ordinary path, where the rate limiter still covers it and the parsed type
// still dispatches it.
//
// The Contains pre-filter is exact, not heuristic: topLevelFrameType refuses a
// value carrying any escape, so a line it classifies as `datagram` contains
// those bytes literally. It keeps the structural walk off every line of the
// hottest receive path, where the answer is almost always no.
func isDatagramWireLine(line string) bool {
	if !strings.Contains(line, protocol.DatagramFrameType) {
		return false
	}
	claimed, named := topLevelFrameType(line)
	return named && claimed == protocol.DatagramFrameType
}

// reportDatagramResidueUnreachable records the one thing that must not happen:
// protocol.ParseFrameLine resolved a line to `datagram` that classifyFrameLine
// had called neither a datagram nor ambiguous, so neither the diversion nor
// admission acted on it and it arrived at a dispatch switch instead.
//
// It is a LOG and a drop rather than a delivery, and that is the point of
// finding 1: delivering it would put the universal parse back in front of the
// neighbour's budget (§4.1 step 1), which is exactly the residue the pre-parse
// classification exists to remove. An Error line is the right volume because
// the condition is a disagreement between two scanners in this repository — it
// cannot be produced by a peer, only by a bug in one of them.
func (s *Service) reportDatagramResidueUnreachable(direction, peer string) {
	s.datagramMetrics.ObserveInbound("", datagram.InboundDropped, datagram.DropMalformed)
	log.Error().
		Str("direction", direction).
		Str("peer", peer).
		Msg("datagram_residue_branch_reached_classifier_disagrees_with_parser")
}

// datagramNeighbour is everything the ingress needs to know about the
// neighbour a frame arrived from, resolved ONCE by the direction-specific
// dispatcher because only IT can answer any of it.
//
// The two identifiers are separate fields because they are separate facts, and
// keeping them apart is the whole point:
//
//   - budgetKey is WHO PAYS. It is either an identity the remote PROVED to
//     this node or the host:port THIS node dialled — never a string the
//     neighbour chose, because a budget keyed on the sender's own claim is
//     reset by the sender at will (see AdmissionKeySpace);
//   - identity is the NAME the neighbour presents, and on an outbound session it
//     is an unproven claim: the challenge of that handshake travels the other way
//     (authenticatePeerSession signs OURS, nothing signs theirs), so the
//     welcome's address is a label. It is not proof of WHO the neighbour is, and
//     since this round it is not what channel-relative state keys on either;
//   - channel is WHICH SOCKET, and it is a third field for the reason the first
//     two are two. A PeerIdentity names a NODE, so keying reverse records, their
//     per-upstream quota and the return path of an answer on it let a neighbour
//     that wrote somebody else's fingerprint into its welcome share that node's
//     state and pull that node's answers. domain.ConnID is exactly "which
//     socket": it is minted from ONE node-wide monotonic counter on both
//     directions, it is stable for the life of the connection, and nothing a peer
//     sends can change it.
//
// The layer derives the LEVEL of proof from the first two rather than being told
// it a fourth time: an arrival whose budgetKey is
// datagram.ProvenIdentityKey(identity) is the only one it reads as proven, so
// the two fields BOTH have to name the same neighbour in the proven namespace
// before a type that DECLARED it needs a proven neighbour is delivered to
// (datagram.Pipeline.senderProofGate). A dialled session cannot produce that
// pair, which is the whole point: nothing on that direction is proven, so naming
// a stranger in the welcome buys the sender nothing.
//
// speaksPlane is the negotiated set of THIS connection, not of the address:
// during a reconnect the address holds a different connection with a different
// handshake (sessionHasCapability).
type datagramNeighbour struct {
	label     string
	budgetKey datagram.AdmissionKey
	channel   datagram.ChannelID
	identity  domain.PeerIdentity
	direction datagramDirection
	// speaksPlane reports whether mesh_datagram_v1 was negotiated on the
	// connection this frame arrived on.
	speaksPlane bool
}

// sessionDatagramNeighbour resolves the neighbour behind an OUTBOUND session.
//
// The budget key is the dial address and nothing else. session.address is
// assigned in the session's struct literal and never written again, so unlike
// peerIdentity it needs no lock and cannot be observed half-written by a read
// loop that started before the handshake finished.
//
// The CHANNEL is session.connID and deliberately NOT the address: the address
// survives a reconnect and the connection does not, while channel-relative state
// belongs to one socket — a reverse record whose upstream outlived the session it
// was taken on would return its answer over a connection the question never
// arrived on. connID is assigned in the same struct literal as address and never
// rewritten, so it needs no lock for the same reason.
func (s *Service) sessionDatagramNeighbour(session *peerSession) datagramNeighbour {
	return datagramNeighbour{
		direction: datagramOutboundSession,
		budgetKey: datagram.DialedAddressKey(session.address),
		channel:   datagram.NetworkChannel(session.connID),
		// Read through the accessor for the ordering reason sessionPeerIdentity
		// documents: the read loop is running before the handshake writes it.
		identity:    s.sessionPeerIdentity(session),
		label:       string(session.address),
		speaksPlane: s.sessionHasCapability(session, domain.CapMeshDatagramV1),
	}
}

// inboundDatagramNeighbour resolves the neighbour behind an ACCEPTED
// connection, where the identity IS proven: connauth.VerifyAuthSession checked
// an Ed25519 signature over a challenge this node generated, made with a key
// whose fingerprint is that very identity, so the budget key and the conveyor's
// identity are the same fact.
//
// It reads the verified auth state rather than the NetCore identity mirror
// because the mirror is written on one branch of handleAuthSession only (the
// one that also resolved an overlay address); the auth state is where the proof
// lives, and a neighbour must not become unbillable because its address was not
// resolvable.
func (s *Service) inboundDatagramNeighbour(connID domain.ConnID) datagramNeighbour {
	identity := s.provenInboundPeerIdentity(connID)
	return datagramNeighbour{
		direction:   datagramInbound,
		budgetKey:   datagram.ProvenIdentityKey(identity),
		channel:     datagram.NetworkChannel(connID),
		identity:    identity,
		label:       identity.String(),
		speaksPlane: s.connHasCapability(connID, domain.CapMeshDatagramV1),
	}
}

// provenInboundPeerIdentity returns the identity an accepted connection PROVED
// during auth, or the zero identity when nothing was proven.
func (s *Service) provenInboundPeerIdentity(connID domain.ConnID) domain.PeerIdentity {
	hello, verified := s.authenticatedAddressForConn(connID)
	if !verified {
		return domain.PeerIdentity{}
	}
	return domain.PeerIdentityFromWire(hello.Address)
}

// inboundDatagramBudgetKey is the neighbour an accepted connection's datagram
// bytes are billed to. It exists so the pre-parse refusals of the inbound
// reader bill the same key the ingress does — two keys for one connection is
// two budgets, and a neighbour would spend both.
func (s *Service) inboundDatagramBudgetKey(connID domain.ConnID) datagram.AdmissionKey {
	return datagram.ProvenIdentityKey(s.provenInboundPeerIdentity(connID))
}

// dispatchSessionDatagramLine hands ONE raw datagram line arriving on an
// outbound session to the ingress.
//
// It is reached from ONE place — the read loop's pre-parse diversion — and
// stays a function of its own because the peer accounting it performs is what
// servePeerSession and dispatchPeerSessionFrame would have applied on the way
// to the inbox: diverting before the inbox must not make a datagram invisible
// to peer health.
//
// The capability gate that used to stand here now stands INSIDE the ingress,
// below the charge (§4.1 step 1): refusing above it made the cheapest verdict
// this direction has also its only free one, so a neighbour that authenticated
// without mesh_datagram_v1 could hold the socket at line rate for nothing.
func (s *Service) dispatchSessionDatagramLine(session *peerSession, line string) {
	if session == nil {
		// Without the session there is no dial address, hence no key that is
		// ours rather than the sender's — dropping is the only honest answer.
		return
	}
	address := session.address
	s.markPeerRead(address, protocol.Frame{Type: protocol.DatagramFrameType})
	s.markPeerUsefulReceive(address)
	neighbour := s.sessionDatagramNeighbour(session)
	result := s.handleDatagramFrame(s.runCtx, line, neighbour)
	if result.BanWorthy() {
		// The outbound side has no ConnID-keyed ban surface — the ban score is
		// charged per accepted connection (addBanScore keys on the inbound
		// conn's IP). A header violation from a peer we dialled is therefore
		// recorded rather than scored; the frame is already dropped, and the
		// session-level defences (control-frame rate limit, disconnect-storm
		// quarantine) remain the operative controls on this direction.
		log.Warn().
			Err(result.Err()).
			Str("peer", string(address)).
			Str("budget_key", neighbour.budgetKey.String()).
			Str("claimed_identity", neighbour.identity.String()).
			Msg("datagram_header_violation_outbound_session")
	}
}

// dispatchInboundDatagramLine hands ONE raw datagram line arriving on an
// accepted connection to the ingress, and charges the ban score the layer
// asked for.
//
// Reached from the pre-parse diversion in dispatchNetworkFrame, through
// dispatchInboundDatagramWire. It reports whether the frame was ACCEPTED, not
// whether the connection survives — for this plane the connection always does:
// §2 makes a datagram this node will not process a silent drop, never an error
// frame and never a tear-down.
//
// The capability gate moved INTO the ingress, below the charge, for the reason
// stated on dispatchSessionDatagramLine: a refusal above the budget is a
// refusal the neighbour gets for free, and it was free on both directions.
func (s *Service) dispatchInboundDatagramLine(connID domain.ConnID, wire string) bool {
	result := s.handleDatagramFrame(s.runCtx, wire, s.inboundDatagramNeighbour(connID))
	if result.BanWorthy() {
		// §4.4: punishment is reserved for violations of the stable header and
		// auth, which every datagram transit is obliged to check. An unknown header
		// version and an unimplemented dtype never reach this branch.
		s.addBanScore(connID, banIncrementInvalidSig)
	}
	return result.Accepted()
}

// dispatchInboundDatagramWire is the pre-parse entry of the inbound command
// plane: everything dispatchNetworkFrame does around a `datagram` case, minus
// the universal parse it exists to skip.
//
// The three steps it repeats are repeated because they are the frame-type
// independent obligations of that function, not because the case needed them
// duplicated: the activity touch keeps the staleness check honest, the auth
// gate answers auth_required for a command that exists on this port but needs
// a session (`datagram` is in p2pWireCommands for exactly that), and the
// protocol_trace line is what an operator greps a receive path by.
//
// It always keeps the connection: a datagram this node will not process is a
// silent drop by contract (§2), never an error frame and never a tear-down.
func (s *Service) dispatchInboundDatagramWire(connID domain.ConnID, addr string, wire string) bool {
	accepted := false
	defer func() {
		log.Debug().
			Str("protocol", "json/tcp").
			Str("addr", addr).
			Str("direction", "recv").
			Str("command", protocol.DatagramFrameType).
			Bool("accepted", accepted).
			Msg("protocol_trace")
	}()

	s.touchConnActivity(connID)
	if !s.isConnAuthenticated(connID) {
		_ = s.sendFrameViaNetworkSync(s.runCtx, connID, protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired})
		return false
	}
	accepted = s.dispatchInboundDatagramLine(connID, wire)
	return true
}

// handleDatagramFrame is the SINGLE ingress of the datagram plane. Both
// network directions end here with the raw wire line they read, and nothing
// between the socket and this function is allowed to re-encode it: §3.4
// requires the strict parser to see the original bytes, because duplicate
// keys, unknown fields and canonical encodings are all invisible after a
// round trip through the universal Frame.
//
// The order of the gates is fixed by §2.3 and §4.1:
//
//  1. the neighbour must be BILLABLE — the dispatcher must have produced a key
//     that is either proven or ours. This is the only step above the charge,
//     and it is allowed there because it does not read the frame at all: it is
//     a property of the connection, decided before the line arrived;
//  2. the charge of §4.1 step 1: bytes and frames of the FULL wire line,
//     before any decoding and before EVERY refusal below. A refusal that runs
//     above the charge is a refusal the neighbour gets for free, and the
//     cheapest verdict on the node makes the best flood;
//  3. the capability of THIS connection. A neighbour that never negotiated
//     mesh_datagram_v1 is dropped silently — no error frame, no tear-down, no
//     ban score, whatever the dtype, class or payload — because §2 makes every
//     refusal of this plane silent and §4.4 reserves punishment for the stable
//     header and auth;
//  4. the two things the conveyor is told about the neighbour: the NAME it
//     presents and the CHANNEL it arrived on. A frame carrying neither is
//     dropped, having paid. They are separate gates because they are separate
//     facts — the name is what the §7 seams are shown, the channel is what every
//     channel-relative decision keys on — and on an outbound session only the
//     channel is defensible. "Not proven" is not a warning left to the reader:
//     the value the seams receive answers Identity() only where the budget key
//     itself names this identity as proven, and a type that DECLARED it needs
//     that proof is not delivered to at all on such a direction;
//  5. the strict MaxFrameLine budget, counted on the raw line INCLUDING its
//     newline. The inbound command reader already caps at 128 KiB, but the
//     peer-session reader accepts up to 8 MiB, and without this the claim
//     "a frame is smaller than 128 KiB" would stop being a property of
//     reception;
//  6. the strict parser, on the raw bytes.
//
// Past those gates the frame goes to the conveyor, which re-runs the strict
// parse itself: §4.1 step 1 charges the neighbour's admission budget on the
// RAW bytes before anything is decoded, so the parse cannot be hoisted above
// it. That is why the line — not a parsed frame — is what crosses this
// boundary, and why nothing above changes when the conveyor is absent.
//
// THIS FUNCTION IS THE ONLY PLACE STAGE ONE IS CHARGED, and it is the only
// place that CAN be: two of the gates above — the plane and the size — never
// reach the conveyor, so a charge below them would leave each of them free. The
// conveyor declares no stage-one seam at all for that reason, and the key
// charged here is handed down to it (InboundOpts.BudgetKey) so stage two of §5
// lands in the same bucket instead of deriving one from a neighbour that may
// have proven nothing.
//
// Without a conveyor (the feature flag off, or its construction failed) the
// function stops at the parse. It counts the frame honestly — nothing was
// delivered, forwarded or answered — and the reason is unknown_dtype, which is
// literally the state of such a build: there is no type registry, so every
// dtype is one it does not implement, and §2 makes that a silent drop on a
// live connection with no ban. Nothing is charged either, because there is no
// budget to charge; in practice the path is unreachable from the network,
// since a node without the plane never advertises mesh_datagram_v1.
func (s *Service) handleDatagramFrame(ctx context.Context, line string, neighbour datagramNeighbour) datagramIngressResult {
	direction := neighbour.direction
	if neighbour.budgetKey.IsZero() {
		return s.refuseDatagram(direction, datagram.DropMalformed, false, errDatagramUnbillablePeer)
	}

	layer := s.datagramLayer()
	if layer != nil && !layer.admission.Admit(neighbour.budgetKey, len(line)) {
		return s.refuseDatagram(direction, datagram.DropAdmission, false, nil)
	}

	if !neighbour.speaksPlane {
		return s.dropDatagramOffThePlane(neighbour)
	}
	if neighbour.identity.IsZero() {
		return s.refuseDatagram(direction, datagram.DropMalformed, false, errDatagramPeerUnidentified)
	}
	if neighbour.channel.IsZero() {
		// A dispatcher that could name no connection has nothing for the conveyor
		// to key channel-relative state on, and the only value left to fall back
		// to is the presented identity — which is the defect the channel exists to
		// remove. Both dispatchers derive it from a ConnID assigned at
		// construction, so this is a wiring fault of THIS node rather than peer
		// misbehaviour: no ban, and the frame has already been charged.
		return s.refuseDatagram(direction, datagram.DropMalformed, false, errDatagramNoChannel)
	}
	if exceedsDatagramFrameLine(line) {
		// A frame this large cannot have been emitted by a conforming
		// sender: the writer's own budget is the same 128 KiB. On the
		// peer-session path it means the peer pushed a multi-megabyte
		// line through the wide response reader, which is the exact
		// attack §2.3 closes. The bytes have been charged above; what the
		// refusal costs beyond that is stated in one place
		// (refuseOversizeDatagram), and it is nothing.
		return s.refuseOversizeDatagram(direction, neighbour.budgetKey, wireLineBudget(line))
	}

	if layer != nil {
		return datagramIngressResultOf(layer.pipeline.HandleInbound(ctx, datagram.InboundOpts{
			Line: []byte(line),
			Peer: neighbour.identity,
			// The socket this line arrived on. Everything the conveyor keys on the
			// CHANNEL — the upstream of a reverse record, its share of the
			// per-upstream quota, the return path of an answer — keys on THIS and
			// never on Peer above, which on an outbound session is the neighbour's
			// own claim.
			Channel: neighbour.channel,
			// The key stage one was just charged on, carried down so the
			// verification budget of §5 cannot land anywhere else. On an
			// outbound session Peer above is the neighbour's own claim; this is
			// the host:port THIS node dialled.
			BudgetKey: neighbour.budgetKey,
		}))
	}

	frame, err := protocol.ParseDatagramFrameLine(line)
	if err != nil {
		return s.refuseDatagram(direction, datagramDropReasonFor(err), datagramBanWorthy(err), err)
	}
	s.datagramMetrics.ObserveInbound(frame.Mode, datagram.InboundDropped, datagram.DropUnknownDType)
	log.Debug().
		Str("direction", direction.String()).
		Str("budget_key", neighbour.budgetKey.String()).
		Str("mode", frame.Mode.String()).
		Str("dtype", frame.DType.String()).
		Msg("datagram_parsed_no_pipeline")
	return datagramIngressResult{accepted: true}
}

// dropDatagramOffThePlane refuses a frame that arrived on a connection whose
// handshake never established the datagram plane — the peer did not advertise
// mesh_datagram_v1, or this node did not, since the negotiated set is the
// intersection of the two (intersectCapabilities).
//
// The neighbour has ALREADY been charged by the caller, which is the whole
// change: the drop costs the sender exactly what the frame cost this node to
// read. Beyond that the refusal is as quiet as §2 requires — no error frame, no
// tear-down, no ban score — and the rule does not look at the dtype, the class
// or the payload, because none of them can make a plane exist on a connection
// that never negotiated it.
//
// It deliberately does NOT go through refuseDatagram. That helper counts an
// inbound OUTCOME, and Metrics.Observed means "frames the conveyor decided on";
// a frame refused above the plane was never handed to it, and counting it there
// would make the two disagree. ObserveDrop is the seam for exactly this shape —
// it moves the reason breakdown of §10 and leaves the inbound totals alone — so
// the drop is now in the ledger under a reason of its own without becoming an
// observation.
//
// The counter is not a weakening of §2. "Silent" there is a property of the
// WIRE: the neighbour still learns nothing, is still not disconnected and is
// still not scored. What it must not also be is invisible to the operator — a
// neighbour off the plane pushing frames at line rate was, until this counter,
// indistinguishable from ordinary load in the §10 ledger, with only a Debug line
// nobody has enabled to say otherwise.
func (s *Service) dropDatagramOffThePlane(neighbour datagramNeighbour) datagramIngressResult {
	s.datagramMetrics.ObserveDrop(datagram.DropPlaneNotNegotiated)
	log.Debug().
		Str("direction", neighbour.direction.String()).
		Str("budget_key", neighbour.budgetKey.String()).
		Str("peer", neighbour.label).
		Msg("datagram_dropped_plane_not_negotiated")
	return datagramIngressResult{err: errDatagramPlaneNotNegotiated}
}

// datagramIngressResultOf maps the conveyor's verdict onto the dispatcher's.
//
// The two types carry the same three facts, and the mapping is deliberately
// total rather than a subset: "accepted" is every non-drop outcome, because
// the dispatcher's only use for the flag is the frame-accounting counter, and
// a stored or answered frame was accepted by every reading of the word. The
// ban verdict is passed through untouched — §4.4 decides it inside the layer,
// where the reason lives, and re-deriving it here from the drop reason is how
// the two surfaces would come to disagree.
//
// The conveyor already counted the outcome through the SAME metrics instance
// this file's refusal paths use, so nothing is counted again here.
func datagramIngressResultOf(result datagram.InboundResult) datagramIngressResult {
	return datagramIngressResult{
		err:      result.Err(),
		accepted: !result.Dropped(),
		ban:      result.BanWorthy(),
	}
}

// datagramCarriesOwnBudget reports whether a frame type is exempt from the
// general inbound command limiter because the datagram layer charges it
// itself.
//
// The exemption is conditional on the conveyor actually EXISTING, and that
// condition is the whole argument:
//
//   - the command limiter allows 30 frames/s per connection with a burst of
//     100. That is a control-plane budget. A datagram session carries control
//     exchanges AND bulk chunks over the same socket — one 64 KiB bulk frame
//     per chunk — so a file transfer moved onto this plane would be throttled
//     to a rate no legacy path was ever throttled to, and the throttle's
//     answer is not a drop but a connection tear-down with ban points;
//   - the layer's own two-stage admission (§5) is strictly stronger where it
//     matters: it is charged per AUTHENTICATED NEIGHBOUR rather than per
//     socket, it counts BYTES as well as frames — the command limiter counts
//     neither — and its third dimension caps signature verifications, which
//     is what actually bounds the CPU a hostile peer can spend. It also
//     refuses silently, which is what §2 requires of this plane;
//   - and it is charged BEFORE parsing, on the raw line, so an exempt frame
//     is not an uncounted frame for even one step.
//
// Removing the limiter before the layer counted anything would have been a
// regression, which is why M9a left it in place. It is removed now, and only
// now, because the budget that replaces it is wired: no layer, no exemption.
//
// The first argument is the TOP-LEVEL type of the line, classified as
// frameLineNamed by the caller (frameLineExemptFromCommandLimit), and that is a
// contract rather than a convenience: a line that peekFrameType calls a datagram
// while encoding/json calls it something else would otherwise leave the command
// limiter — because it looks like a datagram — and then never reach the layer's
// budget either, because it is not one. The exemption and the pre-parse
// diversion (isDatagramWireLine) therefore answer from ONE classification, so
// the exemption and the diversion cannot disagree about what a line IS.
//
// The second argument is the KEY the §5 budget would be charged on, and it is
// the other half of "the layer charges it itself". A budget nobody can be billed
// for is not a budget: on an accepted connection the key is the identity the
// neighbour PROVED, and before `auth_ok` there is none — the inbound dispatcher
// answers `auth_required` above the ingress, so the line reaches neither budget.
// Exempting it there gave an unauthenticated socket a free, unmetered way to
// make this node build and write a sync error frame per line. The caller passes
// the key its own direction defends (the proven identity inbound, the dialled
// address on an outbound session), so the exemption and the charge cannot
// disagree about who pays.
//
// And the layer now charges EVERY line it diverts, which is what makes the
// exemption sound. The role gate used to stand above the charge — both
// dispatchers refused a connection that never declared mesh_datagram_v1 before
// handleDatagramFrame ran — so a neighbour that authenticated WITHOUT the
// capability was exempted here and charged nowhere. The gate now stands INSIDE
// the ingress, below the charge, so an exempt line is billed whether it is
// served, gated or refused. The same shape still holds for the three older
// members of this exemption (file_command, the bulk announce types), whose
// replacement budgets remain behind their capability checks; that is their
// plane's gap and is recorded rather than papered over.
func (s *Service) datagramCarriesOwnBudget(claimed string, budgetKey datagram.AdmissionKey) bool {
	return claimed == protocol.DatagramFrameType && s.datagramLayer() != nil && !budgetKey.IsZero()
}

// refuseDatagram counts one refusal and shapes the verdict. It exists so the
// metric can never be forgotten on a refusal path: every early return above
// goes through it.
func (s *Service) refuseDatagram(direction datagramDirection, reason datagram.DropReason, ban bool, err error) datagramIngressResult {
	s.datagramMetrics.ObserveInbound("", datagram.InboundDropped, reason)
	log.Debug().
		Err(err).
		Str("direction", direction.String()).
		Str("reason", reason.String()).
		Bool("ban", ban).
		Msg("datagram_refused")
	return datagramIngressResult{err: err, ban: ban}
}

// errDatagramUnbillablePeer marks a datagram whose dispatcher could produce no
// admission key: neither an identity the peer proved nor an address this node
// dialled.
//
// It is the ONE refusal above the charge, and it is reachable — the inbound
// dispatcher checks authentication and then resolves the proof separately, so a
// connection torn down between the two answers here. What makes that acceptable
// is that such a connection is already gone: the neighbour cannot repeat the
// refusal at line rate on a socket the node no longer holds. It is not peer
// misbehaviour either way, hence no ban.
var errDatagramUnbillablePeer = errors.New("datagram: frame from a neighbour with no admission key")

// errDatagramPeerUnidentified marks a frame from a neighbour that HAS a budget
// key but named no identity for the conveyor to key its state on.
//
// It is the outbound session whose welcome carried a blank or unparseable
// address: nothing on that direction is proven, so the address is a label the
// peer chose, and choosing an empty one used to buy the whole session an
// unbudgeted channel. The frame is now charged first and refused after.
var errDatagramPeerUnidentified = errors.New("datagram: frame from a neighbour that named no identity")

// errDatagramPlaneNotNegotiated marks a frame that arrived on a connection
// whose handshake never established mesh_datagram_v1. No ban: the neighbour is
// told nothing, so it can learn nothing from being punished.
var errDatagramPlaneNotNegotiated = errors.New("datagram: the plane was not negotiated on this connection")

// errDatagramNoChannel marks a frame whose dispatcher could name no connection.
//
// It is unreachable through either live path — both derive the channel from a
// domain.ConnID assigned in the connection's own construction and never
// rewritten — and it is checked anyway because the alternative to a named
// refusal here is a conveyor that silently keys channel state on the presented
// identity. No ban: nothing about it is the neighbour's doing.
var errDatagramNoChannel = errors.New("datagram: frame from a neighbour with no transport channel")

// datagramBanWorthy applies the punishment rule of §4.4 to a strict-parser
// refusal.
//
// TWO refusals are explicitly NOT misbehaviour, and they are the two a
// neighbour can be handed by somebody else:
//
//   - an unknown header VERSION: §2 makes a future `v` a silent drop without
//     forwarding, on a connection that stays up, precisely so that a v3 frame
//     crossing a v2 node costs its sender nothing;
//   - a line past MaxFrameLine: the size is a verdict about the LINE, and the
//     relay that forwarded it did not write the frame inside it
//     (refuseOversizeDatagram states the rule and the ingress applies it above
//     this parser; the check here keeps the two answers from diverging).
//
// Every other refusal from this parser is a violation of the STABLE header — a
// duplicate key, an unknown field, a non-canonical encoding, a value outside its
// bounds, a mode/class combination the matrix forbids — and those are what every
// datagram transit is obliged to check, so charging for them cannot hit an
// honest relay.
//
// Note what is NOT reachable here and therefore never punished: an unknown
// dtype and a malformed payload schema. The parser does not interpret either
// one; they are the endpoint's business, and §4.4 keeps them ban-free.
func datagramBanWorthy(err error) bool {
	return !errors.Is(err, protocol.ErrDatagramUnknownVersion) &&
		!errors.Is(err, protocol.ErrFrameTooLarge)
}

// datagramDropReasonFor maps a strict-parser refusal onto the layer's drop
// reason, so the counters this milestone increments are the same ones the
// pipeline increments when it takes the parse over.
func datagramDropReasonFor(err error) datagram.DropReason {
	switch {
	case errors.Is(err, protocol.ErrDatagramUnknownVersion):
		return datagram.DropUnknownHeaderVersion
	case errors.Is(err, protocol.ErrFrameTooLarge):
		return datagram.DropFrameTooLarge
	default:
		return datagram.DropMalformed
	}
}
